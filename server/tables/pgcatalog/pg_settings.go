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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/config"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgSettingsName is a constant to the pg_settings name.
const PgSettingsName = "pg_settings"

// InitPgSettings handles registration of the pg_settings handler.
func InitPgSettings() {
	tables.AddHandler(PgCatalogName, PgSettingsName, PgSettingsHandler{})
}

// PgSettingsHandler is the handler for the pg_settings table.
type PgSettingsHandler struct{}

var _ tables.Handler = PgSettingsHandler{}

// Name implements the interface tables.Handler.
func (p PgSettingsHandler) Name() string {
	return PgSettingsName
}

// RowIter implements the interface tables.Handler.
func (p PgSettingsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rows := make([]sql.Row, 0, len(pgSettingsSupportedParameters))
	for _, name := range pgSettingsSupportedParameters {
		row, err := pgSettingsRow(ctx, name)
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	return &pgSettingsRowIter{rows: rows}, nil
}

// Schema implements the interface tables.Handler.
func (p PgSettingsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgSettingsSchema,
		PkOrdinals: nil,
	}
}

// pgSettingsSchema is the schema for pg_settings.
var pgSettingsSchema = sql.Schema{
	{Name: "name", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "setting", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "unit", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "category", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "short_desc", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "extra_desc", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "context", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "vartype", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "source", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "min_val", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "max_val", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "enumvals", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "boot_val", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "reset_val", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "sourcefile", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "sourceline", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgSettingsName},
	{Name: "pending_restart", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgSettingsName},
}

// pgSettingsRowIter is the sql.RowIter for the pg_settings table.
type pgSettingsRowIter struct {
	rows []sql.Row
	idx  int
}

var _ sql.RowIter = (*pgSettingsRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgSettingsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.rows) {
		return nil, io.EOF
	}
	iter.idx++
	return iter.rows[iter.idx-1], nil
}

// Close implements the interface sql.RowIter.
func (iter *pgSettingsRowIter) Close(ctx *sql.Context) error {
	return nil
}

var pgSettingsSupportedParameters = []string{
	"server_version_num",
	"wal_sender_timeout",
}

func pgSettingsRow(ctx *sql.Context, name string) (sql.Row, error) {
	sysVar, globalValue, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return nil, nil
	}
	parameter, ok := sysVar.(*config.Parameter)
	if !ok {
		return nil, nil
	}
	value, err := ctx.GetSessionVariable(ctx, name)
	if err != nil || value == nil {
		value = globalValue
	}

	return sql.Row{
		parameter.Name,                 // name
		fmt.Sprint(value),              // setting
		pgSettingsUnit(parameter.Name), // unit
		parameter.Category,             // category
		parameter.ShortDesc,            // short_desc
		nil,                            // extra_desc
		string(parameter.Context),      // context
		pgSettingsVarType(parameter),   // vartype
		string(parameter.Source),       // source
		nil,                            // min_val
		nil,                            // max_val
		nil,                            // enumvals
		fmt.Sprint(parameter.Default),  // boot_val
		fmt.Sprint(parameter.ResetVal), // reset_val
		nil,                            // sourcefile
		nil,                            // sourceline
		false,                          // pending_restart
	}, nil
}

func pgSettingsUnit(name string) any {
	switch name {
	case "wal_sender_timeout":
		return "ms"
	default:
		return nil
	}
}

func pgSettingsVarType(parameter *config.Parameter) string {
	switch parameter.GetDefault().(type) {
	case bool:
		return "bool"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	default:
		return "string"
	}
}
