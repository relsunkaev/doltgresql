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
	"cmp"
	"slices"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAvailableExtensionsName is a constant to the pg_available_extensions name.
const PgAvailableExtensionsName = "pg_available_extensions"

// InitPgAvailableExtensions handles registration of the pg_available_extensions handler.
func InitPgAvailableExtensions() {
	tables.AddHandler(PgCatalogName, PgAvailableExtensionsName, PgAvailableExtensionsHandler{})
}

// PgAvailableExtensionsHandler is the handler for the pg_available_extensions table.
type PgAvailableExtensionsHandler struct{}

var _ tables.Handler = PgAvailableExtensionsHandler{}

// Name implements the interface tables.Handler.
func (p PgAvailableExtensionsHandler) Name() string {
	return PgAvailableExtensionsName
}

// RowIter implements the interface tables.Handler.
func (p PgAvailableExtensionsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	availableExtensions := extensions.GetAvailableExtensions()
	names := mapsKeys(availableExtensions)
	slices.Sort(names)
	rows := make([]sql.Row, 0, len(names))
	for _, name := range names {
		ext := availableExtensions[name]
		var installedVersion any
		if loaded, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(name)); err != nil {
			return nil, err
		} else if loaded.ExtName.IsValid() {
			installedVersion = loaded.LibIdentifier.Version().String()
		}
		rows = append(rows, sql.Row{
			name,
			ext.Control.DefaultVersion.String(),
			installedVersion,
			nullableString(ext.Control.Comment),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgAvailableExtensionsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAvailableExtensionsSchema,
		PkOrdinals: nil,
	}
}

// pgAvailableExtensionsSchema is the schema for pg_available_extensions.
var pgAvailableExtensionsSchema = sql.Schema{
	{Name: "name", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgAvailableExtensionsName},
	{Name: "default_version", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAvailableExtensionsName},
	{Name: "installed_version", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAvailableExtensionsName}, // TODO: collation C
	{Name: "comment", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAvailableExtensionsName},
}

func mapsKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
