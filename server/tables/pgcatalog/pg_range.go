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

// PgRangeName is a constant to the pg_range name.
const PgRangeName = "pg_range"

// InitPgRange handles registration of the pg_range handler.
func InitPgRange() {
	tables.AddHandler(PgCatalogName, PgRangeName, PgRangeHandler{})
}

// PgRangeHandler is the handler for the pg_range table.
type PgRangeHandler struct{}

var _ tables.Handler = PgRangeHandler{}

// Name implements the interface tables.Handler.
func (p PgRangeHandler) Name() string {
	return PgRangeName
}

// RowIter implements the interface tables.Handler.
func (p PgRangeHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(pgBuiltinRangeRows()...), nil
}

func pgBuiltinRangeRows() []sql.Row {
	return []sql.Row{
		pgBuiltinRangeRow("int4range", pgtypes.Int32.ID, "int4multirange", "int4_ops", "int4range_canonical", "int4range_subdiff"),
		pgBuiltinRangeRow("int8range", pgtypes.Int64.ID, "int8multirange", "int8_ops", "int8range_canonical", "int8range_subdiff"),
		pgBuiltinRangeRow("numrange", pgtypes.Numeric.ID, "nummultirange", "numeric_ops", "-", "numrange_subdiff"),
		pgBuiltinRangeRow("daterange", pgtypes.Date.ID, "datemultirange", "date_ops", "daterange_canonical", "daterange_subdiff"),
		pgBuiltinRangeRow("tsrange", pgtypes.Timestamp.ID, "tsmultirange", "timestamp_ops", "-", "tsrange_subdiff"),
		pgBuiltinRangeRow("tstzrange", pgtypes.TimestampTZ.ID, "tstzmultirange", "timestamptz_ops", "-", "tstzrange_subdiff"),
	}
}

func pgBuiltinRangeRow(rangeName string, subtype id.Type, multirangeName string, subtypeOpclass string, canonical string, subtypeDiff string) sql.Row {
	return sql.Row{
		id.NewType("pg_catalog", rangeName).AsId(), // rngtypid
		subtype.AsId(), // rngsubtype
		id.NewType("pg_catalog", multirangeName).AsId(), // rngmultitypid
		zeroOID(), // rngcollation
		pgCatalogOpclassID(indexmetadata.AccessMethodBtree, subtypeOpclass), // rngsubopc
		canonical,   // rngcanonical
		subtypeDiff, // rngsubdiff
	}
}

// Schema implements the interface tables.Handler.
func (p PgRangeHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgRangeSchema,
		PkOrdinals: nil,
	}
}

// pgRangeSchema is the schema for pg_range.
var pgRangeSchema = sql.Schema{
	{Name: "rngtypid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRangeName},
	{Name: "rngsubtype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRangeName},
	{Name: "rngmultitypid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRangeName},
	{Name: "rngcollation", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRangeName},
	{Name: "rngsubopc", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRangeName},
	{Name: "rngcanonical", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgRangeName}, // TODO: regproc type
	{Name: "rngsubdiff", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgRangeName},   // TODO: regproc type
}

// pgRangeRowIter is the sql.RowIter for the pg_range table.
type pgRangeRowIter struct {
}

var _ sql.RowIter = (*pgRangeRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgRangeRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgRangeRowIter) Close(ctx *sql.Context) error {
	return nil
}
