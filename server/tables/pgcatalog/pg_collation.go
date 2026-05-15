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
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgCollationName is a constant to the pg_collation name.
const PgCollationName = "pg_collation"

// InitPgCollation handles registration of the pg_collation handler.
func InitPgCollation() {
	tables.AddHandler(PgCatalogName, PgCollationName, PgCollationHandler{})
}

// PgCollationHandler is the handler for the pg_collation table.
type PgCollationHandler struct{}

var _ tables.Handler = PgCollationHandler{}

// Name implements the interface tables.Handler.
func (p PgCollationHandler) Name() string {
	return PgCollationName
}

// RowIter implements the interface tables.Handler.
func (p PgCollationHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &pgCollationRowIter{idx: 0}, nil
}

// Schema implements the interface tables.Handler.
func (p PgCollationHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     PgCollationSchema,
		PkOrdinals: nil,
	}
}

// PgCollationSchema is the schema for pg_collation.
var PgCollationSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collprovider", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collisdeterministic", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collencoding", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgCollationName},
	{Name: "collcollate", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgCollationName},   // TODO: collation C
	{Name: "collctype", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgCollationName},     // TODO: collation C
	{Name: "colliculocale", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgCollationName}, // TODO: collation C
	{Name: "collversion", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgCollationName},   // TODO: collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgCollationName},
}

// pgCollationRowIter is the sql.RowIter for the pg_collation table.
type pgCollationRowIter struct {
	idx int
}

var _ sql.RowIter = (*pgCollationRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgCollationRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(pgCollationRows) {
		return nil, io.EOF
	}
	iter.idx++
	collation := pgCollationRows[iter.idx-1]
	return collation.toRow(), nil
}

// Close implements the interface sql.RowIter.
func (iter *pgCollationRowIter) Close(ctx *sql.Context) error {
	return nil
}

type pgCollation struct {
	oid       id.Id
	name      string
	provider  string
	encoding  int32
	collate   any
	ctype     any
	icuLocale any
	version   any
}

var pgCollationRows = []pgCollation{
	{
		oid:      id.NewCollation("pg_catalog", indexmetadata.CollationDefault).AsId(),
		name:     indexmetadata.CollationDefault,
		provider: "d",
		encoding: -1,
	},
	{
		oid:      id.NewCollation("pg_catalog", indexmetadata.CollationC).AsId(),
		name:     indexmetadata.CollationC,
		provider: "c",
		encoding: -1,
		collate:  indexmetadata.CollationC,
		ctype:    indexmetadata.CollationC,
	},
	{
		oid:      id.NewCollation("pg_catalog", indexmetadata.CollationPOSIX).AsId(),
		name:     indexmetadata.CollationPOSIX,
		provider: "c",
		encoding: -1,
		collate:  indexmetadata.CollationPOSIX,
		ctype:    indexmetadata.CollationPOSIX,
	},
	{
		oid:      id.NewCollation("pg_catalog", indexmetadata.CollationUcsBasic).AsId(),
		name:     indexmetadata.CollationUcsBasic,
		provider: "c",
		encoding: 6,
		collate:  indexmetadata.CollationC,
		ctype:    indexmetadata.CollationC,
	},
	{
		oid:       id.NewCollation("pg_catalog", indexmetadata.CollationUndIcu).AsId(),
		name:      indexmetadata.CollationUndIcu,
		provider:  "i",
		encoding:  -1,
		icuLocale: "und",
	},
}

func (collation pgCollation) toRow() sql.Row {
	return sql.Row{
		collation.oid,                         // oid
		collation.name,                        // collname
		id.NewNamespace("pg_catalog").AsId(),  // collnamespace
		id.NewId(id.Section_User, "postgres"), // collowner
		collation.provider,                    // collprovider
		true,                                  // collisdeterministic
		collation.encoding,                    // collencoding
		collation.collate,                     // collcollate
		collation.ctype,                       // collctype
		collation.icuLocale,                   // colliculocale
		collation.version,                     // collversion
		id.NewTable(PgCatalogName, PgCollationName).AsId(), // tableoid
	}
}
