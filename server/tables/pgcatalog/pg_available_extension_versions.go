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
	"slices"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAvailableExtensionVersionsName is a constant to the pg_available_extension_versions name.
const PgAvailableExtensionVersionsName = "pg_available_extension_versions"

// InitPgAvailableExtensionVersions handles registration of the pg_available_extension_versions handler.
func InitPgAvailableExtensionVersions() {
	tables.AddHandler(PgCatalogName, PgAvailableExtensionVersionsName, PgAvailableExtensionVersionsHandler{})
}

// PgAvailableExtensionVersionsHandler is the handler for the pg_available_extension_versions table.
type PgAvailableExtensionVersionsHandler struct{}

var _ tables.Handler = PgAvailableExtensionVersionsHandler{}

// Name implements the interface tables.Handler.
func (p PgAvailableExtensionVersionsHandler) Name() string {
	return PgAvailableExtensionVersionsName
}

// RowIter implements the interface tables.Handler.
func (p PgAvailableExtensionVersionsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
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
		installed := false
		if loaded, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(name)); err != nil {
			return nil, err
		} else if loaded.ExtName.IsValid() && loaded.LibIdentifier.Version() == ext.Control.DefaultVersion {
			installed = true
		}
		rows = append(rows, sql.Row{
			name,
			ext.Control.DefaultVersion.String(),
			installed,
			ext.Control.Superuser,
			ext.Control.Trusted,
			ext.Control.Relocatable,
			nullableString(ext.Control.Schema),
			stringSliceToAny(ext.Control.Requires),
			nullableString(ext.Control.Comment),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgAvailableExtensionVersionsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAvailableExtensionVersionsSchema,
		PkOrdinals: nil,
	}
}

// pgAvailableExtensionVersionsSchema is the schema for pg_available_extension_versions.
var pgAvailableExtensionVersionsSchema = sql.Schema{
	{Name: "name", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "version", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "installed", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "superuser", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "trusted", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "relocatable", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "schema", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "requires", Type: pgtypes.NameArray, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
	{Name: "comment", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAvailableExtensionVersionsName},
}
