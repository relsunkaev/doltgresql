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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	pgext "github.com/dolthub/doltgresql/core/extensions/pg_extension"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgExtensionName is a constant to the pg_extension name.
const PgExtensionName = "pg_extension"

// InitPgExtension handles registration of the pg_extension handler.
func InitPgExtension() {
	tables.AddHandler(PgCatalogName, PgExtensionName, PgExtensionHandler{})
}

// PgExtensionHandler is the handler for the pg_extension table.
type PgExtensionHandler struct{}

var _ tables.Handler = PgExtensionHandler{}

// Name implements the interface tables.Handler.
func (p PgExtensionHandler) Name() string {
	return PgExtensionName
}

// RowIter implements the interface tables.Handler.
func (p PgExtensionHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}

	loadedExtensions := extCollection.GetLoadedExtensions(ctx)
	if !extCollection.HasLoadedExtension(ctx, id.NewExtension("plpgsql")) {
		loadedExtensions = append(loadedExtensions, extensions.Extension{
			ExtName:       id.NewExtension("plpgsql"),
			Namespace:     id.NewNamespace("pg_catalog"),
			Owner:         "postgres",
			Relocatable:   false,
			LibIdentifier: extensions.CreateLibraryIdentifier("plpgsql", pgext.ToVersion(1, 0)),
		})
	}
	rows := make([]sql.Row, 0, len(loadedExtensions))
	for _, ext := range loadedExtensions {
		namespace := ext.Namespace
		if !id.Id(namespace).IsValid() {
			namespace = id.NewNamespace("public")
		}
		owner := ext.Owner
		if owner == "" {
			owner = "postgres"
		}
		rows = append(rows, sql.Row{
			ext.ExtName.AsId(),                   // oid
			ext.ExtName.Name(),                   // extname
			id.NewId(id.Section_User, owner),     // extowner
			namespace.AsId(),                     // extnamespace
			ext.Relocatable,                      // extrelocatable
			ext.LibIdentifier.Version().String(), // extversion
			nil,                                  // extconfig
			nil,                                  // extcondition
			id.NewTable(PgCatalogName, PgExtensionName).AsId(), // tableoid
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgExtensionHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgExtensionSchema,
		PkOrdinals: nil,
	}
}

// pgExtensionSchema is the schema for pg_extension.
var pgExtensionSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgExtensionName},
	{Name: "extname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgExtensionName},
	{Name: "extowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgExtensionName},
	{Name: "extnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgExtensionName},
	{Name: "extrelocatable", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgExtensionName},
	{Name: "extversion", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgExtensionName}, // TODO: collation C
	{Name: "extconfig", Type: pgtypes.OidArray, Default: nil, Nullable: true, Source: PgExtensionName},
	{Name: "extcondition", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgExtensionName}, // TODO: collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgExtensionName, Hidden: true},
}
