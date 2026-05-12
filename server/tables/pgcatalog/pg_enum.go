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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgEnumName is a constant to the pg_enum name.
const PgEnumName = "pg_enum"

// InitPgEnum handles registration of the pg_enum handler.
func InitPgEnum() {
	tables.AddHandler(PgCatalogName, PgEnumName, PgEnumHandler{})
}

// PgEnumHandler is the handler for the pg_enum table.
type PgEnumHandler struct{}

var _ tables.Handler = PgEnumHandler{}

// Name implements the interface tables.Handler.
func (p PgEnumHandler) Name() string {
	return PgEnumName
}

// RowIter implements the interface tables.Handler.
func (p PgEnumHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var rows []sql.Row
	err = collection.IterateTypes(ctx, func(typ *pgtypes.DoltgresType) (bool, error) {
		if typ.TypType != pgtypes.TypeType_Enum {
			return false, nil
		}
		labels := make([]pgtypes.EnumLabel, 0, len(typ.EnumLabels))
		for _, label := range typ.EnumLabels {
			labels = append(labels, label)
		}
		sort.Slice(labels, func(i, j int) bool {
			if labels[i].SortOrder == labels[j].SortOrder {
				return labels[i].ID.Label() < labels[j].ID.Label()
			}
			return labels[i].SortOrder < labels[j].SortOrder
		})
		for _, label := range labels {
			rows = append(rows, sql.Row{
				label.ID.AsId(),
				typ.ID.AsId(),
				label.SortOrder,
				label.ID.Label(),
			})
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgEnumHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgEnumSchema,
		PkOrdinals: nil,
	}
}

// pgEnumSchema is the schema for pg_enum.
var pgEnumSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgEnumName},
	{Name: "enumtypid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgEnumName},
	{Name: "enumsortorder", Type: pgtypes.Float32, Default: nil, Nullable: false, Source: PgEnumName},
	{Name: "enumlabel", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgEnumName},
}

// TODO: add unique constraint "pg_enum_typid_label_index"

// pgEnumRowIter is the sql.RowIter for the pg_enum table.
type pgEnumRowIter struct {
}

var _ sql.RowIter = (*pgEnumRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgEnumRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgEnumRowIter) Close(ctx *sql.Context) error {
	return nil
}
