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

	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAiosName is a constant to the pg_aios name.
const PgAiosName = "pg_aios"

// InitPgAios handles registration of the pg_aios handler.
func InitPgAios() {
	tables.AddHandler(PgCatalogName, PgAiosName, PgAiosHandler{})
}

// PgAiosHandler is the handler for the pg_aios table.
type PgAiosHandler struct{}

var _ tables.Handler = PgAiosHandler{}

// Name implements the interface tables.Handler.
func (p PgAiosHandler) Name() string {
	return PgAiosName
}

// RowIter implements the interface tables.Handler.
func (p PgAiosHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// PkSchema implements the interface tables.Handler.
func (p PgAiosHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAiosSchema,
		PkOrdinals: nil,
	}
}

// pgAiosSchema is the schema for pg_aios.
var pgAiosSchema = sql.Schema{
	{Name: "pid", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "io_id", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "io_generation", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "state", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "operation", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "off", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "length", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "target", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "handle_data_len", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "raw_result", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "result", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "target_desc", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "f_sync", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "f_localmem", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAiosName},
	{Name: "f_buffered", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAiosName},
}
